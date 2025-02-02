using Order.API.Services;
using Scalar.AspNetCore;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();
// Register the IBus interface and the Bus class. We use the AddSingleton because we want to have a single instance of the Bus class. 
builder.Services.AddSingleton<IBus, Bus>();
builder.Services.AddScoped<OrderService>();

var app = builder.Build();

await app.CreateTopicsOrQueuesAsync();
// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
    app.MapScalarApiReference();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
