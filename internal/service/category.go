package service

import (
	"context"
	"io"

	empty "github.com/golang/protobuf/ptypes/empty"

	"github.com/jvmMachado/go-grpc/database"
	"github.com/jvmMachado/go-grpc/internal/pb"
)

type CategoryService struct {
	pb.UnimplementedCategoryServiceServer
	CategoryDB database.Category
}

func NewCategoryService(categoryDB database.Category) *CategoryService {
	return &CategoryService{CategoryDB: categoryDB}
}

func (c *CategoryService) CreateCategory(
	ctx context.Context,
	in *pb.CreateCategoryRequest,
) (*pb.Category, error) {
	category, err := c.CategoryDB.CreateCategory(in.Name, in.Description)
	if err != nil {
		return nil, err
	}
	categoryResponse := &pb.Category{
		Id:          category.ID,
		Name:        category.Name,
		Description: category.Description,
	}
	return categoryResponse, nil
}

func (c *CategoryService) ListCategories(ctx context.Context, in *empty.Empty) (*pb.CategoryList, error) {
	categories, err := c.CategoryDB.GetCategories()
	if err != nil {
		return nil, err
	}
	var categoryList []*pb.Category
	for _, category := range categories {
		categoryList = append(categoryList, &pb.Category{
			Id:          category.ID,
			Name:        category.Name,
			Description: category.Description,
		})
	}
	return &pb.CategoryList{Categories: categoryList}, nil
}

func (c *CategoryService) GetCategory(ctx context.Context, in *pb.CategoryGetRequest) (*pb.Category, error) {
	category, err := c.CategoryDB.GetCategory(in.Id)
	if err != nil {
		return nil, err
	}
	categoryResponse := &pb.Category{
		Id:          category.ID,
		Name:        category.Name,
		Description: category.Description,
	}
	return categoryResponse, nil
}

func (c *CategoryService) CreateCategoryStream(
	stream pb.CategoryService_CreateCategoryStreamServer,
) error {
	categories := &pb.CategoryList{}

	for {
		category, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(categories)
		}
		if err != nil {
			return err
		}

		createdCategory, err := c.CategoryDB.CreateCategory(category.Name, category.Description)
		if err != nil {
			return err
		}

		categories.Categories = append(categories.Categories, &pb.Category{
			Id:          createdCategory.ID,
			Name:        createdCategory.Name,
			Description: createdCategory.Description,
		})
	}
}

func (c *CategoryService) CreateCategoryStreamBidirectional(
	stream pb.CategoryService_CreateCategoryStreamBidirectionalServer,
) error {
	for {
		categoryStream, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		category, err := c.CategoryDB.CreateCategory(categoryStream.Name, categoryStream.Description)
		if err != nil {
			return err
		}

		err = stream.Send(&pb.Category{Id: category.ID, Name: category.Name, Description: category.Description})
		if err != nil {
			return err
		}
	}
}
